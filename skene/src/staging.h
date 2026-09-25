#pragma once
// Internal: where the two-pass writer (design R9) keeps section bodies between
// passes, and where it writes the finished file.
//
// Pass 1 (add_row_group) encodes a row group and STAGES each column node's
// section bodies in arrival order. Pass 2 (finish) lays the file out
// column-major and copies every body from the stage to its final offset. The
// staging mode follows the output:
//
//   output to a caller's buffer  -> stage in memory, one buffer per (node,
//      region). The output is in memory anyway; each node's buffer is released
//      as soon as it is copied out, so the peak stays ~one file, not two.
//   output to a path             -> stage in a SCRATCH FILE the caller names,
//      and stream the output to disk. Memory stays at one row group of plans
//      plus the directory, whatever the file size — the reason for two passes.
//
// Neither mode is a fallback for the other: the caller's choice of output
// decides, and a scratch path given with buffer output is rejected.

#include <cstdint>
#include <string>
#include <vector>

#include "skene/status.h"

namespace skene {

class Sink;

class Stage {
  public:
    Stage() = default;
    ~Stage();
    Stage(const Stage&) = delete;
    Stage& operator=(const Stage&) = delete;

    void   open_memory();
    Status open_file(const std::string& path);   // created exclusively; removed on close

    // Appends `bytes` to node `node`'s data (index == false) or index stream and
    // returns where it was staged — an offset only this stage can interpret.
    Status append(uint32_t node, bool index, const void* data, size_t bytes,
                  uint64_t* out_offset);

    // Copies staged bytes to the sink.
    Status copy_to(uint32_t node, bool index, uint64_t offset, uint64_t bytes,
                   Sink* sink);

    // Frees a node's stream once it has been copied out (memory mode only).
    void release(uint32_t node, bool index);

    // Bytes staged so far, across every node — what a caller watches to decide
    // a file is big enough.
    uint64_t staged_bytes() const noexcept { return staged_; }

    void close();

  private:
    bool                              file_mode_ = false;
    int                               fd_ = -1;
    std::string                       path_;
    uint64_t                          file_end_ = 0;
    std::vector<std::vector<uint8_t>> data_;
    std::vector<std::vector<uint8_t>> index_;
    uint64_t                          staged_ = 0;
};

// The finished file's destination: a caller's vector, or a file written to
// `<path>.skene-partial` and renamed into place by commit() — so a reader sees
// the file complete or not at all, as write_file() guarantees.
class Sink {
  public:
    Sink() = default;
    ~Sink();
    Sink(const Sink&) = delete;
    Sink& operator=(const Sink&) = delete;

    void   open_memory(std::vector<uint8_t>* out);
    Status open_file(const std::string& path);

    uint64_t position() const noexcept { return position_; }
    Status   write(const void* data, size_t bytes);
    Status   zeros(size_t bytes);
    Status   commit();     // file mode: flush, close, rename into place
    void     abandon();    // file mode: close and remove the partial file

  private:
    Status flush();

    std::vector<uint8_t>* memory_ = nullptr;
    int                   fd_ = -1;
    std::string           path_;
    std::string           partial_;
    std::vector<uint8_t>  buffer_;     // file mode write buffer
    uint64_t              position_ = 0;
};

}  // namespace skene
