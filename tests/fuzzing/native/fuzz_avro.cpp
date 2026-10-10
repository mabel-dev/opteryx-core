// Fuzz rugo's Avro reader: arbitrary bytes in, no crash out.
//
// The whole decode path is pure C++ (docs/AVRO_READER_DESIGN.md): container header
// and block framing, the four block codecs, the schema parser (yyjson), the
// compiled decode program and the interpreter that writes Draken buffers. Every
// length, count and union/enum index in an Avro file is a varint the reader did not
// write, so this is where the bounds arithmetic lives.
//
// Two reads per input: every top-level column (exercises the read and JSON ops), and
// a dotted projection (exercises the skip ops and the nullable-record null path).
// The column buffers are owned by AvroRead and freed when it goes out of scope.
//
// The oracle is the sanitizer, not the return value: a refused file is the reader
// working.

#include <cstddef>
#include <cstdint>
#include <exception>
#include <string>
#include <vector>

#include "avro/avro_reader.hpp"

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    using namespace rugo::avro;

    try {
        AvroRead out;
        read_avro_buffer(data, size, {}, true, std::string(), out);
    } catch (const std::exception&) {
    } catch (...) {
    }

    try {
        AvroRead out;
        read_avro_buffer(data, size, {"data_file.file_path", "data_file.lower_bounds", "c"}, false, std::string(), out);
    } catch (const std::exception&) {
    } catch (...) {
    }

    // Reader-schema resolution against the seed corpus's schema: a promotion, a
    // reordered nested record rendered as JSON, defaults and a reader-only constant.
    static const std::string reader = R"({"type":"record","name":"r","fields":[
        {"name":"c","type":"double"},
        {"name":"added","type":"string","default":"x"},
        {"name":"e","type":{"type":"enum","name":"E","symbols":["b","a","z"]}},
        {"name":"data_file","type":["null",{"type":"record","name":"D","fields":[
            {"name":"lower_bounds","type":{"type":"array","items":{"type":"record","name":"KV","fields":[
                {"name":"value","type":"bytes"},{"name":"key","type":"long"}]}}},
            {"name":"file_path","type":"string"},
            {"name":"n","type":["null","long"],"default":null}]}]}]})";
    try {
        AvroRead out;
        read_avro_buffer(data, size, {}, true, reader, out);
    } catch (const std::exception&) {
    } catch (...) {
    }

    return 0;
}
