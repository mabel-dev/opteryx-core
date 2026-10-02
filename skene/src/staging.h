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
//   output to a caller's stream  -> the LEAD column node (node 0) is not staged
//      at all: its data sections go straight to the stream as they are encoded.
//      Every other node stages in memory. For where neither a file nor memory
//      can hold the output (a Cloud Run worker, whose disk IS memory) — see
//      FileWriter::begin(options, OutputStream*, prefix).
//
// Neither mode is a fallback for the other: the caller's choice of output
// decides, and a scratch path given with buffer output is rejected.

#include <cstdint>
#include <string>
#include <vector>

#include "skene/status.h"
#include "skene/writer.h"   // OutputStream

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
    // Memory staging, except node 0's DATA sections, which are written straight
    // to `lead` at the next kSectionAlign boundary of its position. Their staging
    // offset is that position, relative to the start of the stream.
    void   open_lead_stream(Sink* lead);
    bool   streams_lead() const noexcept { return lead_ != nullptr; }

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
    Sink*                             lead_ = nullptr;
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
    // Buffered writes to a caller's stream. position() starts at 0 and counts
    // the stream's bytes until rebase() says where the stream sits in the file.
    void   open_stream(OutputStream* stream);
    // Stream mode: every byte written so far, and from now on, lies `base`
    // bytes further into the file than the stream position — the length of the
    // prefix the writer returns separately.
    void   rebase(uint64_t base) noexcept { position_ += base; }

    uint64_t position() const noexcept { return position_; }
    Status   write(const void* data, size_t bytes);
    Status   zeros(size_t bytes);
    Status   commit();     // file mode: flush, close, rename into place
    void     abandon();    // file mode: close and remove the partial file

  private:
    Status flush();

    std::vector<uint8_t>* memory_ = nullptr;
    OutputStream*         stream_ = nullptr;
    int                   fd_ = -1;
    std::string           path_;
    std::string           partial_;
    std::vector<uint8_t>  buffer_;     // file mode write buffer
    uint64_t              position_ = 0;
};

}  // namespace skene
