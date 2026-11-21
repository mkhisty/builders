#pragma once
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace shopping {
// A record ends only at LF; quotes wrap a whole field and never escape content.
inline bool parse(std::string_view line, std::vector<std::string_view>& fields, size_t count) {
    fields.clear();
    size_t start = 0;
    while (true) {
        size_t end = line.find(',', start);
        auto field = line.substr(start, end == line.npos ? line.size() - start : end - start);
        if (field.size() >= 2 && field.front() == '"' && field.back() == '"')
            field = field.substr(1, field.size() - 2);
        if (field.find('"') != field.npos) return false;
        fields.push_back(field);
        if (end == line.npos) break;
        if (fields.size() >= count) return false;
        start = end + 1;
    }
    return fields.size() == count;
}

template<class F> void read(const std::filesystem::path& path, size_t count, F consume) {
    std::ifstream input(path, std::ios::binary);
    if (!input) throw std::runtime_error("Cannot read " + path.string());
    std::string line;
    std::vector<std::string_view> fields;
    std::getline(input, line); // header
    while (std::getline(input, line)) if (parse(line, fields, count)) consume(fields);
    if (input.bad()) throw std::runtime_error("Read failed: " + path.string());
}

inline void field(std::ostream& output, std::string_view value) {
    output.put('"'); output.write(value.data(), value.size()); output.put('"');
}

}
