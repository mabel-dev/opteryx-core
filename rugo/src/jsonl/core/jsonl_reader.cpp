#include "jsonl_reader.hpp"
#include "structural_scan.hpp"
#include "volnitsky.h"     // SPIKE: raw prefilter
#include <cstdio>
#include <cstring>
#include <stdexcept>
#include <utility>

namespace rugo::_jsonl {

PrefilterResult volnitsky_prefilter(
    const uint8_t* buffer, size_t length,
    const uint8_t* needle, size_t needle_len) {
    PrefilterResult r;
    if (length < needle_len || needle_len < 2) return r;
    VolnitskyTable* t = volnitsky_alloc();
    volnitsky_build(t, needle, needle_len);
    r.candidates.reserve(length / 16);

    // Single whole-buffer Volnitsky pass: the bigram table skips ~needle_len-1 bytes across
    // every non-matching window, so a rare needle leaps over whole records. On a hit, copy
    // the enclosing line and jump past it (handles dedup of multiple hits in one record).
    size_t last_end = 0; bool any = false;
    for (size_t p = needle_len - 1; p < length; ) {
        const uint16_t h = (static_cast<uint16_t>(buffer[p - 1]) << 8) | buffer[p];
        const uint16_t k = t->entries[h];
        if (!k) { p += needle_len - 1; continue; }
        const size_t hs = p - k;
        if (hs + needle_len <= length && std::memcmp(buffer + hs, needle, needle_len) == 0
            && (!any || hs >= last_end)) {
            size_t ls = hs; while (ls > 0 && buffer[ls - 1] != '\n') --ls;
            size_t le = hs; while (le < length && buffer[le] != '\n') ++le;
            r.candidates.insert(r.candidates.end(), buffer + ls, buffer + le);
            r.candidates.push_back('\n');
            ++r.matched_records;
            last_end = le; any = true;
            p = (le + 1 > needle_len - 1) ? le + 1 : needle_len - 1;  // skip past the matched line
            continue;
        }
        p += 1;
    }
    volnitsky_free(t);
    return r;
}

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

    // Determine how much of the buffer holds COMPLETE records. A record is complete
    // only when terminated by a newline; on a non-final chunk the bytes after the last
    // newline are a partial record straddling the 64MB boundary. They must be carried
    // to the next chunk, NOT parsed/counted here — otherwise that record is counted
    // twice (once as a partial here, once when completed in the next chunk). On the
    // final chunk (eof) the trailing newline-less record IS complete, so process all.
    size_t process_len = read_buffer.size();
    if (!eof) {
        for (size_t i = markers.size(); i-- > 0; ) {
            if (markers[i].marker_type == static_cast<uint8_t>(MarkerType::NEWLINE)) {
                process_len = markers[i].position + 1;
                markers.resize(i + 1);  // drop markers belonging to the carried tail
                break;
            }
        }
        // No newline in a non-final chunk => one record spans the whole buffer; process
        // it anyway to make progress (the >chunk-size-record edge keeps prior behaviour).
    }

    // Step 2: Interpret only the complete-record prefix.
    InterpreterResult interp_result = interpret_jsonl(
        read_buffer.data(),
        process_len,
        markers,
        context,
        predictor
    );

    // Step 3: Extract column names from projected columns or all found keys
    if (!context.projected_columns.empty()) {
        result.column_names = context.projected_columns;
    } else {
        // Extract unique column names from first record
        if (interp_result.all_records.num_records() > 0) {
            const RecordView first_record = interp_result.all_records[0];
            for (const auto& field : first_record) {
                std::string key_name(
                    reinterpret_cast<const char*>(read_buffer.data() + field.key_start),
                    field.key_width
                );
                result.column_names.push_back(key_name);
            }
        }
    }

    // Step 4: Records + the parsed prefix of the buffer (FieldSpans index into it).
    // interp_result is local — move the record structure out instead of copying it.
    result.num_records = interp_result.num_records_passed;
    result.records = std::move(interp_result.all_records);
    result.buffer_data.assign(read_buffer.begin(), read_buffer.begin() + process_len);

    // Step 5: Carry the un-parsed tail (the partial boundary record) to the next chunk.
    if (process_len < read_buffer.size()) {
        unconsumed_bytes.assign(read_buffer.begin() + process_len, read_buffer.end());
    } else {
        unconsumed_bytes.clear();
    }

    result.success = true;
    return result;
}

}  // namespace rugo::_jsonl
