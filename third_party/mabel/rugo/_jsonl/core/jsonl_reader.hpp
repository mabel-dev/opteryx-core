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

    // FieldSpans for each record: [record_idx][field_idx]
    std::vector<std::vector<FieldSpan>> records;

    // Inferred schema (if applicable)
    std::map<std::string, std::string> inferred_schema;

    // Number of records that passed predicates
    size_t num_records = 0;
};

// JsonlReader: handles buffering, chunking, unconsumed bytes
// User calls next_chunk() repeatedly until done
class JsonlReader {
public:
    // Open a file for reading
    JsonlReader(const std::string& file_path, const ParseContext& context);

    // OR open from pre-loaded bytes/buffer
    JsonlReader(const uint8_t* buffer, size_t length, const ParseContext& context);

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
