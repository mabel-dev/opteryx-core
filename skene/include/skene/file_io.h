#pragma once
// skene/file_io.h — read and write .skene files on a local filesystem.
//
// Deliberately thin. The writer produces a complete, self-contained byte image
// and the reader consumes one, so persistence is a separate concern from the
// format — which is what lets the same bytes go to a local file, to object
// storage, or straight into a spill buffer without the format knowing.
//
// Whole-file I/O. Reading a column at a time from a local file is possible (the
// footer gives absolute extents) but there is no caller for it yet, and an
// unused seek path is an untested seek path.

#include <cstdint>
#include <string>
#include <vector>

#include "skene/status.h"

namespace skene {

// Writes `bytes` to `path`, replacing it. Writes to a temporary alongside the
// target and renames on success, so a reader never observes a half-written
// file: rename is atomic within a filesystem, a partial write is not.
Status write_file(const std::string& path, const std::vector<uint8_t>& bytes);

// Reads the whole file. Fails loud if it is smaller than the smallest possible
// well-formed .skene file — cheaper than letting the reader discover it, and a
// clearer message.
Status read_file(const std::string& path, std::vector<uint8_t>* out);

}  // namespace skene
