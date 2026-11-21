#pragma once
#include "csv.hpp"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

namespace fs = std::filesystem;
using i64 = std::int64_t;
using shopping::read;
using shopping::field;

std::vector<fs::path> files(const fs::path& dir) {
    std::vector<fs::path> result;
    for (const auto& entry : fs::directory_iterator(dir))
        if (entry.is_regular_file() && entry.path().extension() == ".csv") result.push_back(entry.path());
    std::sort(result.begin(), result.end());
    return result;
}

bool digit(char c) { return c >= '0' && c <= '9'; }
int number(std::string_view s, size_t at, size_t length) {
    int value = 0;
    for (size_t i = at; i < at + length; ++i) {
        if (!digit(s[i])) return -1;
        value = value * 10 + s[i] - '0';
    }
    return value;
}

// Gregorian civil date to days since 1970-01-01, including pre-epoch dates.
i64 days(int year, int month, int day) {
    year -= month <= 2;
    int era = year / 400;
    unsigned y = year - era * 400;
    unsigned doy = (153 * (month + (month > 2 ? -3 : 9)) + 2) / 5 + day - 1;
    return era * 146097LL + y * 365 + y / 4 - y / 100 + doy - 719468;
}

bool timestamp(std::string_view s, i64& value) {
    if (s.empty()) return false;
    if (std::all_of(s.begin(), s.end(), digit)) {
        value = 0;
        for (char c : s) {
            if (value > (std::numeric_limits<i64>::max() - (c - '0')) / 10) return false;
            value = value * 10 + c - '0';
        }
        return true;
    }
    if (s.size() != 20 && s.size() != 25) return false;
    if (s[4] != '-' || s[7] != '-' || s[10] != 'T' || s[13] != ':' || s[16] != ':') return false;
    int y = number(s, 0, 4), m = number(s, 5, 2), d = number(s, 8, 2);
    int h = number(s, 11, 2), minute = number(s, 14, 2), second = number(s, 17, 2);
    if (y < 1 || m < 1 || m > 12 || d < 1 || h < 0 || h > 23 || minute < 0 || minute > 59 || second < 0 || second > 59) return false;
    static constexpr int lengths[] = {0,31,28,31,30,31,30,31,31,30,31,30,31};
    bool leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    if (d > lengths[m] + (m == 2 && leap)) return false;
    int offset = 0;
    if (s.size() == 20) {
        if (s[19] != 'Z') return false;
    } else {
        if ((s[19] != '+' && s[19] != '-') || s[22] != ':') return false;
        int oh = number(s, 20, 2), om = number(s, 23, 2);
        if (oh < 0 || oh > 23 || om < 0 || om > 59) return false;
        offset = (oh * 60 + om) * (s[19] == '+' ? 1 : -1);
    }
    value = ((days(y, m, d) * 24 + h) * 60 + minute - offset) * 60000 + second * 1000;
    return true;
}

struct Ids {
    std::unordered_map<std::string, size_t> index;
    std::vector<size_t> parent, rank, smallest;
    std::vector<std::string> names;
    size_t add(std::string_view name) {
        auto [it, added] = index.emplace(std::string(name), names.size());
        if (added) {
            parent.push_back(names.size());
            smallest.push_back(names.size());
            rank.push_back(0);
            names.emplace_back(name);
        }
        return it->second;
    }
    size_t root(size_t x) {
        while (parent[x] != x) { parent[x] = parent[parent[x]]; x = parent[x]; }
        return x;
    }
    void join(std::string_view a, std::string_view b) {
        size_t x = add(a), y = add(b);
        x = root(x); y = root(y);
        if (x == y) return;
        if (rank[x] < rank[y]) std::swap(x, y);
        parent[y] = x;
        if (rank[x] == rank[y]) ++rank[x];
        if (names[smallest[y]] < names[smallest[x]]) smallest[x] = smallest[y];
    }
    void flatten() {
        std::vector<size_t> canonical(parent.size());
        for (size_t i = 0; i < parent.size(); ++i) canonical[i] = smallest[root(i)];
        parent = std::move(canonical);
    }
    std::string_view person(std::string_view id) const {
        // Transparent lookup avoids allocating a string on every event.
        auto it = lookup.find(id);
        return it == lookup.end() ? id : std::string_view(names[it->second]);
    }
    std::unordered_map<std::string_view, size_t> lookup;
    void prepare() {
        flatten();
        lookup.reserve(names.size());
        for (size_t i = 0; i < names.size(); ++i) lookup.emplace(names[i], parent[i]);
        // Only canonical names and the immutable lookup are needed while processing rows.
        index.clear();
        index.rehash(0);
        std::vector<size_t>().swap(parent);
        std::vector<size_t>().swap(rank);
        std::vector<size_t>().swap(smallest);
    }
};

