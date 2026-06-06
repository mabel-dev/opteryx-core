#ifndef _JSONL_READER_HPP_
#define _JSONL_READER_HPP_

#include <vector>
#include <string>
#include <cstdint>
#include <memory>

#include "parse_context.hpp"
#include "field_span.hpp"

namespace rugo::_jsonl {

// Result of reading one chunk
struct ReadResult {
    bool success = false;
    std::string error_message;

    // Column names (in order of ParseContext.projected_columns)
    std::vector<std::string> column_names;

    // Flat-arena document map for this chunk's records.
    RecordSet records;

    // Raw buffer data (needed for FieldSpan offset resolution)
    std::vector<uint8_t> buffer_data;

    // Inferred schema (if applicable)
    std::map<std::string, std::string> inferred_schema;

    // Number of records that passed predicates
    size_t num_records = 0;
};

// SPIKE: Sparser-style raw prefilter for string-equality. Walks records (newline-split)
// and keeps only lines that contain `needle` (a value-anchored, formatting-invariant byte
// pattern, e.g. the JSON-encoded quoted value `"abc-123"`), using Volnitsky substring
// search. SOUND for string equality: a matching record always contains those bytes, so we
// never drop a real match; false positives (the bytes appear elsewhere) are verified away
// downstream. `candidates` is the concatenation of surviving lines, ready to re-read.
struct PrefilterResult {
    std::vector<uint8_t> candidates;     // surviving lines, newline-terminated
    size_t total_records   = 0;
    size_t matched_records = 0;
};
PrefilterResult volnitsky_prefilter(
    const uint8_t* buffer, size_t length,
    const uint8_t* needle, size_t needle_len);

// JsonlReader: handles buffering, chunking, unconsumed bytes
// User calls next_chunk() repeatedly until done
class JsonlReader {
public:
    // Open a file for reading
    JsonlReader(const std::string& file_path, const ParseContext& context);

    // OR open from pre-loaded bytes/buffer
    JsonlReader(const uint8_t* buffer, size_t length, const ParseContext& context);

    // Destructor
    ~JsonlReader();

    // Get next chunk of parsed records
    // Returns success=false when EOF or error
    ReadResult next_chunk();

    // Current state
    bool is_eof() const { return eof; }
    bool has_error() const { return !error_message.empty(); }
    const std::string& get_error() const { return error_message; }

private:
    static constexpr size_t CHUNK_SIZE = 64 * 1024 * 1024;  // 64MB

    // Input source
    enum class SourceType { File, Buffer };
    SourceType source_type;
    std::string file_path;
    FILE* file_handle = nullptr;
    const uint8_t* buffer_data = nullptr;
    size_t buffer_length = 0;
    size_t buffer_offset = 0;

    // State
    std::vector<uint8_t> read_buffer;
    std::vector<uint8_t> unconsumed_bytes;
    OrdinalPredictor predictor;
    ParseContext context;
    bool eof = false;
    std::string error_message;

    // Internal helpers
    bool read_next_chunk_from_source();
    ReadResult process_buffer();
};

}  // namespace rugo::_jsonl

#endif  // _JSONL_READER_HPP_
