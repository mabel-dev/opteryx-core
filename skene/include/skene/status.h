#pragma once
// skene/status.h — the error model.
//
// Fail fast, fail clean. Every failure is explicit, carries an actionable
// message, and stops the read. There is no degraded path, no "best effort"
// parse, and no fallback: this format memcpys buffers and rebuilds absolute
// pointers, so continuing past a detected inconsistency is memory corruption,
// not a wrong answer.
//
// Status codes rather than exceptions across the API surface, so skene can be
// consumed from a nogil / no-exception context without a translation layer.

#include <string>
#include <utility>

namespace skene {

enum class Code {
    kOk = 0,

    // Rejected before ANY content is interpreted.
    kNotSkene,          // magic missing at head or tail
    kTruncated,         // object shorter than its own declared extents
    kUnsupportedVersion,// outside [kMinReadVersion, kVersion]
    kWrongEndianness,   // written by a differently-ordered machine
    kUnknownChecksum,   // checksum algorithm this build cannot verify
    kChecksumMismatch,  // body does not match its recorded checksum

    // Rejected during structural validation, before any buffer is built.
    kMalformed,         // internally contradictory directory
    kUnsupportedSection,// unrecognised REQUIRED section kind
    kUnsupportedEncoding,// unrecognised encoding on a required section
    kUnsupportedType,   // DrakenType this build cannot materialize

    kOutOfMemory,
};

class Status {
  public:
    Status() noexcept : code_(Code::kOk) {}
    Status(Code code, std::string message) : code_(code), message_(std::move(message)) {}

    static Status ok() noexcept { return Status(); }

    bool is_ok() const noexcept { return code_ == Code::kOk; }
    Code code() const noexcept { return code_; }
    const std::string& message() const noexcept { return message_; }

  private:
    Code        code_;
    std::string message_;
};

}  // namespace skene

// Propagate a failing Status unchanged. Deliberately not a try/catch idiom:
// control flow through exceptions is banned, and a swallowed error is worse
// than a crash.
#define SKENE_RETURN_IF_ERROR(expr)                 \
    do {                                            \
        ::skene::Status _skene_st = (expr);         \
        if (!_skene_st.is_ok()) return _skene_st;   \
    } while (0)
