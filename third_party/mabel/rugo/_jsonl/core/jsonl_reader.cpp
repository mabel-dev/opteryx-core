#include "jsonl_reader.hpp"
#include "structural_scan.hpp"
#include <cstdio>
#include <stdexcept>

namespace rugo::_jsonl {

JsonlReader::JsonlReader(const std::string& file_path, const ParseContext& context)
    : source_type(SourceType::File), file_path(file_path), context(context) {
    file_handle = std::fopen(file_path.c_str(), "rb");
    if (!file_handle) {
        error_message = "Failed to open file: " + file_path;
        return;
    }
    read_buffer.reserve(CHUNK_SIZE);
}

JsonlReader::JsonlReader(const uint8_t* buffer, size_t length, const ParseContext& context)
    : source_type(SourceType::Buffer), buffer_data(buffer), buffer_length(length),
      context(context) {
    read_buffer.reserve(CHUNK_SIZE);
}

JsonlReader::~JsonlReader() {
    if (file_handle) {
        std::fclose(file_handle);
    }
}

ReadResult JsonlReader::next_chunk() {
    ReadResult result;

    if (eof || !error_message.empty()) {
        result.success = false;
        result.error_message = error_message.empty() ? "EOF" : error_message;
        return result;
    }

    // Read next chunk from source
    if (!read_next_chunk_from_source()) {
        result.success = false;
        result.error_message = error_message;
        return result;
    }

    // Process the buffer
    return process_buffer();
}

bool JsonlReader::read_next_chunk_from_source() {
    read_buffer.clear();
    read_buffer.insert(read_buffer.end(), unconsumed_bytes.begin(), unconsumed_bytes.end());

    if (source_type == SourceType::File) {
        if (!file_handle) {
            error_message = "File not open";
            return false;
        }

        size_t bytes_to_read = CHUNK_SIZE - read_buffer.size();
        if (bytes_to_read > 0) {
            size_t old_size = read_buffer.size();
            read_buffer.resize(old_size + bytes_to_read);
            size_t bytes_read = std::fread(read_buffer.data() + old_size, 1, bytes_to_read, file_handle);
            read_buffer.resize(old_size + bytes_read);

            if (bytes_read < bytes_to_read) {
                if (std::ferror(file_handle)) {
                    error_message = "File read error";
                    return false;
                }
                eof = true;
            }
        }
    } else {
        // Buffer source
        size_t bytes_to_read = std::min((size_t)CHUNK_SIZE - read_buffer.size(),
                                        buffer_length - buffer_offset);
        if (bytes_to_read > 0) {
            read_buffer.insert(read_buffer.end(),
                              buffer_data + buffer_offset,
                              buffer_data + buffer_offset + bytes_to_read);
            buffer_offset += bytes_to_read;
        }

        if (buffer_offset >= buffer_length) {
            eof = true;
        }
    }

    if (read_buffer.empty()) {
        eof = true;
        return false;
    }

    return true;
}

ReadResult JsonlReader::process_buffer() {
    ReadResult result;

    if (read_buffer.empty()) {
        result.success = true;
        result.num_records = 0;
        return result;
    }

    // Step 1: SIMD scan for structural markers
    std::vector<MarkerPosition> markers = scan_structural_markers(
        read_buffer.data(),
        read_buffer.size()
    );

    // Step 2: Interpret buffer with projection/predicates
    InterpreterResult interp_result = interpret_jsonl(
        read_buffer.data(),
        read_buffer.size(),
        markers,
        context,
        predictor
    );

    // Step 3: Extract column names from projected columns or all found keys
    if (!context.projected_columns.empty()) {
        result.column_names = context.projected_columns;
    } else {
        // Extract unique column names from first record
        if (!interp_result.all_records.empty()) {
            const auto& first_record = interp_result.all_records[0];
            for (const auto& field : first_record) {
                std::string key_name(
                    reinterpret_cast<const char*>(read_buffer.data() + field.key_start),
                    field.key_width
                );
                result.column_names.push_back(key_name);
            }
        }
    }

    // Step 4: Copy records and buffer data
    result.records = interp_result.all_records;
    result.num_records = interp_result.num_records_passed;
    result.buffer_data = read_buffer;

    // Step 5: Track unconsumed bytes for next chunk
    if (interp_result.bytes_consumed < read_buffer.size()) {
        unconsumed_bytes.assign(
            read_buffer.begin() + interp_result.bytes_consumed,
            read_buffer.end()
        );
    } else {
        unconsumed_bytes.clear();
    }

    result.success = true;
    return result;
}

}  // namespace rugo::_jsonl
