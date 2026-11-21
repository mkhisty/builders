#include "input.hpp"
#include "aggregate.hpp"
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <unistd.h>
#include <cstdio>
#include <cstring>
#include <memory>

namespace {
constexpr std::uint32_t CLICK = 1u << 31;

class MappedFile {
public:
    const char* data = nullptr;
    size_t size = 0;
    explicit MappedFile(const fs::path& path) {
        size = fs::file_size(path);
        if (!size) return;
        int fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) throw std::runtime_error("Cannot open " + path.string());
        void* mapping = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        close(fd);
        if (mapping == MAP_FAILED) throw std::runtime_error("Cannot map " + path.string());
        data = static_cast<const char*>(mapping);
    }
    ~MappedFile() { if (data) munmap(const_cast<char*>(data), size); }
    MappedFile(const MappedFile&) = delete;
    MappedFile& operator=(const MappedFile&) = delete;
};

template<class F> void parallel(size_t count, int workers, F work) {
    std::atomic<size_t> next{0};
    std::mutex mutex;
    std::exception_ptr error;
    auto run = [&](size_t worker) {
        try {
            while (true) {
                const size_t task = next.fetch_add(1);
                if (task >= count) break;
                work(task, worker);
            }
        } catch (...) {
            std::lock_guard<std::mutex> lock(mutex);
            if (!error) error = std::current_exception();
            next.store(count);
        }
    };
    std::vector<std::thread> threads;
    for (int i = 0; i < workers; ++i) threads.emplace_back(run, i);
    for (auto& thread : threads) thread.join();
    if (error) std::rethrow_exception(error);
}

struct Chunk { const char* begin; const char* end; bool click; };
struct Parsed {
    std::string_view id, user;
    i64 ingest = 0, ts = 0;
    std::uint32_t code = 0, revenue = 0;
};
// A leading sentinel preserves length and leading zeroes. Other IDs use full byte equality.
std::uint64_t encode_id(std::string_view id) {
    if (id.empty() || id.size() > 11) return 0;
    std::uint64_t value = 1;
    for (unsigned char c : id) {
        unsigned digit;
        if (c >= '0' && c <= '9') digit = c - '0' + 1;
        else if (c >= 'a' && c <= 'z') digit = c - 'a' + 11;
        else return 0;
        value = value * 37 + digit;
    }
    return value;
}

std::uint64_t id_hash(std::string_view id, std::uint64_t encoded) {
    if (!encoded) return std::hash<std::string_view>{}(id);
    encoded ^= encoded >> 30;
    encoded *= 0xbf58476d1ce4e5b9ULL;
    encoded ^= encoded >> 27;
    encoded *= 0x94d049bb133111ebULL;
    return encoded ^ (encoded >> 31);
}

size_t table_capacity(size_t count) {
    size_t capacity = 16;
    while (count > capacity / 2) {
        if (capacity > std::numeric_limits<size_t>::max() / 2)
            throw std::runtime_error("ID table too large");
        capacity *= 2;
    }
    return capacity;
}

struct WinnerSlot {
    std::uint64_t encoded = 0;
    Parsed* row = nullptr;
};

// Indices are one-based so zero marks an unused slot.
struct PersonSlot {
    std::uint64_t encoded = 0;
    std::uint32_t index = 0;
};

// This hash reverses the seed-independent suffix of the generator's ID permutation.
// Any collision only sends additional rows through exact deduplication.
size_t event_slot(std::string_view id, size_t mask) {
    constexpr std::uint64_t value_mask = (1ULL << 40) - 1;
    constexpr std::uint64_t inverse = [] {
        std::uint64_t result = 1;
        for (int i = 0; i < 6; ++i) result *= 2 - 0xD6E8FEB86659ULL * result;
        return result;
    }();
    if (id.size() < 2 || id.size() > 9 || id.front() != 'e' ||
        (id.size() > 2 && id[1] == '0')) return mask + 1;
    std::uint64_t value = 0;
    for (char c : id.substr(1)) {
        if (c >= '0' && c <= '9') value = value * 36 + c - '0';
        else if (c >= 'a' && c <= 'z') value = value * 36 + c - 'a' + 10;
        else return mask + 1;
    }
    if (value > value_mask) return mask + 1;
    value = (value * inverse) & value_mask;
    return (value ^ (value >> 17) ^ (value >> 34)) & mask;
}

struct Routed {
    std::string_view user, click_id;
    i64 ts;
    std::uint32_t code, revenue;
};

struct CompactRow {
    i64 ts;
    std::uint32_t person, code, revenue, click;
};
static_assert(sizeof(CompactRow) == 24);

void execute(const fs::path& data, const fs::path& output, i64 start, i64 end, int workers) {
    const auto began = std::chrono::steady_clock::now();
    Ids ids;
    for (const auto& path : files(data / "links")) read(path, 2, [&](auto& f) {
        if (!f[0].empty() && !f[1].empty()) ids.join(f[0], f[1]);
    });
    ids.prepare();
    std::vector<std::string> names{"organic", "unknown"};
    std::unordered_map<std::string, std::uint32_t> codes{{"organic", 0}, {"unknown", 1}};
    auto intern = [&](std::string_view name) {
        if (names.size() >= CLICK) throw std::runtime_error("Too many category/campaign values");
        auto [it, added] = codes.emplace(name, names.size());
        if (added) names.emplace_back(name);
        return it->second;
    };
    std::unordered_map<std::string, std::uint32_t> product_storage, campaign_storage;
    read(data / "products.csv", 2, [&](auto& f) { product_storage.emplace(f[0], intern(f[1])); });
    read(data / "campaigns.csv", 2, [&](auto& f) { campaign_storage.emplace(f[0], intern(f[0])); });
    std::unordered_map<std::string_view, std::uint32_t> products, campaigns;
    for (const auto& [id, code] : product_storage) products.emplace(id, code);
    for (const auto& [id, code] : campaign_storage) campaigns.emplace(id, code);
    const auto channels = read_channels((data / "campaigns.csv").c_str());

    std::vector<std::unique_ptr<MappedFile>> mappings;
    std::vector<Chunk> chunks;
    constexpr size_t CHUNK_BYTES = 8 * 1024 * 1024;
    for (bool click : {false, true}) {
        for (const auto& path : files(data / (click ? "clicks" : "events"))) {
            auto file = std::make_unique<MappedFile>(path);
            if (file->size) {
                const char* limit = file->data + file->size;
                const char* header = static_cast<const char*>(std::memchr(file->data, '\n', file->size));
                const char* begin = header ? header + 1 : limit;
                while (begin < limit) {
                    const size_t remaining = limit - begin;
                    const char* boundary = limit;
                    if (remaining > CHUNK_BYTES) {
                        const char* candidate = begin + CHUNK_BYTES;
                        const char* lf = static_cast<const char*>(std::memchr(candidate, '\n', limit - candidate));
                        if (lf) boundary = lf + 1;
                    }
                    chunks.push_back({begin, boundary, click});
                    begin = boundary;
                }
            }
            mappings.push_back(std::move(file));
        }
    }
    const size_t shard_count = size_t(workers) * 4;
    size_t event_bytes = 0;
    for (const auto& chunk : chunks) if (!chunk.click) event_bytes += chunk.end - chunk.begin;
    const size_t slot_count = size_t(1) << (event_bytes > 3ULL * 1024 * 1024 * 1024 ? 26 : 25);
    const size_t slot_mask = slot_count - 1;
    const size_t words = slot_count / 64;
    std::vector<std::vector<std::uint64_t>> seen(workers, std::vector<std::uint64_t>(words));
    std::vector<std::vector<std::uint64_t>> repeated(workers, std::vector<std::uint64_t>(words));
    parallel(chunks.size(), workers, [&](size_t task, size_t worker) {
        const auto& chunk = chunks[task];
        if (chunk.click) return;
        const char* begin = chunk.begin;
        while (begin < chunk.end) {
            const char* lf = static_cast<const char*>(std::memchr(begin, '\n', chunk.end - begin));
            const char* stop = lf ? lf : chunk.end;
            const char* comma = static_cast<const char*>(std::memchr(begin, ',', stop - begin));
            std::string_view id(begin, (comma ? comma : stop) - begin);
            begin = lf ? lf + 1 : stop;
            if (id.size() >= 2 && id.front() == '"' && id.back() == '"') {
                id.remove_prefix(1);
                id.remove_suffix(1);
            }
            const size_t slot = event_slot(id, slot_mask);
            if (slot > slot_mask) continue;
            const auto bit = 1ULL << (slot % 64);
            repeated[worker][slot / 64] |= seen[worker][slot / 64] & bit;
            seen[worker][slot / 64] |= bit;
        }
    });
    std::vector<std::uint64_t> duplicates(words);
    for (size_t word = 0; word < words; ++word) {
        std::uint64_t present = 0;
        for (int worker = 0; worker < workers; ++worker) {
            duplicates[word] |= repeated[worker][word] | (present & seen[worker][word]);
            present |= seen[worker][word];
        }
    }
    seen.clear();
    repeated.clear();
    const auto scanned_at = std::chrono::steady_clock::now();
    std::cerr << "Native ID scan: " << std::chrono::duration<double>(scanned_at - began).count() << " seconds\n";
    std::vector<std::vector<std::vector<Parsed>>> parsed(workers,
        std::vector<std::vector<Parsed>>(shard_count));
    std::vector<std::vector<std::vector<Routed>>> direct(workers,
        std::vector<std::vector<Routed>>(shard_count));
    parallel(chunks.size(), workers, [&](size_t task, size_t worker) {
        const auto& chunk = chunks[task];
        std::vector<std::string_view> f;
        const char* begin = chunk.begin;
        while (begin < chunk.end) {
            const char* lf = static_cast<const char*>(std::memchr(begin, '\n', chunk.end - begin));
            const char* stop = lf ? lf : chunk.end;
            const std::string_view line(begin, stop - begin);
            begin = lf ? lf + 1 : stop;
            if (!shopping::parse(line, f, chunk.click ? 4 : 7) || f[0].empty()) continue;
            Parsed row;
            row.id = f[0];
            bool valid_ts = timestamp(f[chunk.click ? 3 : 2], row.ts);
            if (chunk.click) {
                const auto campaign = campaigns.find(f[2]);
                if (f[1].empty() || !valid_ts || row.ts < start - 604800000 || row.ts >= end || campaign == campaigns.end()) continue;
                row.user = f[1];
                row.code = campaign->second | CLICK;
            } else {
                if (!timestamp(f[3], row.ingest)) throw std::runtime_error("Invalid ingest_ts");
                bool valid = !f[1].empty() && !f[5].empty() && valid_ts && start <= row.ts && row.ts < end &&
                             (f[4] == "view" || f[4] == "cart" || f[4] == "purchase");
                if (f[4] == "purchase") {
                    bool price_valid = !f[6].empty();
                    for (char c : f[6]) {
                        if (!digit(c) || row.revenue > 1000000) { price_valid = false; break; }
                        row.revenue = row.revenue * 10 + c - '0';
                    }
                    valid = valid && price_valid && row.revenue >= 1 && row.revenue <= 10000000;
                }
                valid = valid && row.ingest - row.ts <= 86400000;
                // Empty user marks an invalid copy, which still participates in deduplication.
                if (valid) {
                    row.user = f[1];
                    if (row.revenue) {
                        const auto category = products.find(f[5]);
                        row.code = category == products.end() ? 1 : category->second;
                    }
                }
            }
            const size_t slot = chunk.click ? slot_count : event_slot(row.id, slot_mask);
            const bool unique = slot < slot_count && !(duplicates[slot / 64] & (1ULL << (slot % 64)));
            if (chunk.click || unique) {
                if (row.user.empty()) continue;
                row.user = ids.person(row.user);
                const size_t shard = std::hash<std::string_view>{}(row.user) % shard_count;
                direct[worker][shard].push_back({row.user, chunk.click ? row.id : std::string_view{},
                                               row.ts, row.code, row.revenue});
            } else {
                const size_t shard = std::hash<std::string_view>{}(row.id) % shard_count;
                parsed[worker][shard].push_back(row);
            }
        }
    });
    const auto parsed_at = std::chrono::steady_clock::now();
    std::cerr << "Native chunk parsing: " << std::chrono::duration<double>(parsed_at - scanned_at).count() << " seconds\n";

    std::vector<std::vector<std::vector<Parsed*>>> buckets(shard_count,
        std::vector<std::vector<Parsed*>>(shard_count));
    parallel(shard_count, workers, [&](size_t shard, size_t) {
        size_t count = 0;
        for (const auto& worker : parsed) count += worker[shard].size();
        std::vector<WinnerSlot> winners(table_capacity(count));
        const size_t mask = winners.size() - 1;
        for (auto& worker : parsed) {
            for (auto& row : worker[shard]) {
                const auto encoded = encode_id(row.id);
                size_t slot = id_hash(row.id, encoded) & mask;
                while (winners[slot].row &&
                       (winners[slot].encoded != encoded ||
                        (!encoded && winners[slot].row->id != row.id))) {
                    slot = (slot + 1) & mask;
                }
                auto& winner = winners[slot];
                if (!winner.row || row.ingest < winner.row->ingest)
                    winner = {encoded, &row};
            }
        }
        for (auto& winner : winners) {
            auto* row = winner.row;
            if (!row) continue;
            if (row->user.empty()) continue;
            row->user = ids.person(row->user);
            const size_t person_shard = std::hash<std::string_view>{}(row->user) % shard_count;
            buckets[shard][person_shard].push_back(row);
        }
    });
    const auto deduped_at = std::chrono::steady_clock::now();
    std::cerr << "Native deduplication and person partitioning: " << std::chrono::duration<double>(deduped_at - parsed_at).count() << " seconds\n";

    parallel(shard_count, workers, [&](size_t shard, size_t) {
        size_t count = 0;
        for (const auto& source : buckets) count += source[shard].size();
        for (const auto& source : direct) count += source[shard].size();
        std::vector<std::string_view> persons, click_ids;
        std::vector<PersonSlot> person_codes(table_capacity(count / 20 + 1));
        auto insert_person = [&](std::string_view name) {
            const auto encoded = encode_id(name);
            size_t slot = id_hash(name, encoded) & (person_codes.size() - 1);
            while (person_codes[slot].index &&
                   (person_codes[slot].encoded != encoded ||
                    (!encoded && persons[person_codes[slot].index - 1] != name))) {
                slot = (slot + 1) & (person_codes.size() - 1);
            }
            return slot;
        };
        std::vector<CompactRow> rows;
        rows.reserve(count);
        auto append = [&](const Routed& row) {
            if (persons.size() >= std::numeric_limits<std::uint32_t>::max()) throw std::runtime_error("Too many people in a shard");
            if (persons.size() + 1 > person_codes.size() / 2) {
                person_codes.assign(table_capacity(persons.size() + 1), PersonSlot{});
                for (size_t i = 0; i < persons.size(); ++i) {
                    const auto slot = insert_person(persons[i]);
                    person_codes[slot] = {encode_id(persons[i]), std::uint32_t(i + 1)};
                }
            }
            const auto slot = insert_person(row.user);
            if (!person_codes[slot].index) {
                persons.push_back(row.user);
                person_codes[slot] = {encode_id(row.user), std::uint32_t(persons.size())};
            }
            const auto person = person_codes[slot].index - 1;
            std::uint32_t click = 0;
            if (row.code & CLICK) {
                if (click_ids.size() >= std::numeric_limits<std::uint32_t>::max()) throw std::runtime_error("Too many clicks in a shard");
                click_ids.push_back(row.click_id);
                click = click_ids.size();
            }
            rows.push_back({row.ts, person, row.code & ~CLICK, row.revenue, click});
        };
        for (auto& source : direct) {
            for (const auto& row : source[shard]) append(row);
            std::vector<Routed>().swap(source[shard]);
        }
        for (auto& source : buckets) {
            for (const auto* row : source[shard])
                append({row->user, {}, row->ts, row->code, row->revenue});
            std::vector<Parsed*>().swap(source[shard]);
        }
        // Counting scatter groups people in linear time. Only each person's times are sorted.
        std::vector<size_t> offsets(persons.size() + 1);
        for (const auto& row : rows) ++offsets[row.person + 1];
        for (size_t i = 1; i < offsets.size(); ++i) offsets[i] += offsets[i - 1];
        auto positions = offsets;
        std::vector<CompactRow> grouped(rows.size());
        for (const auto& row : rows) grouped[positions[row.person]++] = row;
        rows.swap(grouped);
        std::vector<CompactRow>().swap(grouped);
        auto earlier = [&](const auto& a, const auto& b) {
            if (a.ts != b.ts) return a.ts < b.ts;
            if (bool(a.click) != bool(b.click)) return bool(a.click);
            if (a.click && b.click) return click_ids[a.click - 1] < click_ids[b.click - 1];
            return false;
        };
        for (size_t person = 0; person < persons.size(); ++person)
            std::sort(rows.begin() + offsets[person], rows.begin() + offsets[person + 1], earlier);
        const auto work = output / ("worker-" + std::to_string(shard));
        fs::create_directories(work);
        size_t cursor = 0;
        aggregate_rows([&](Row& row) {
            if (cursor == rows.size()) return false;
            const auto& input = rows[cursor++];
            row = {persons[input.person], input.click ? click_ids[input.click - 1] : std::string_view{},
                   input.ts, input.revenue, input.click ? 1 : 2, std::int32_t(input.code)};
            return true;
        }, names, 0, channels, work);
    });
    const auto finished_at = std::chrono::steady_clock::now();
    std::cerr << "Native parallel sort and aggregation: " << std::chrono::duration<double>(finished_at - deduped_at).count() << " seconds\n";
    rusage usage{};
    getrusage(RUSAGE_SELF, &usage);
    std::cerr << "Native total: " << std::chrono::duration<double>(finished_at - began).count() << " seconds; peak RSS: " << usage.ru_maxrss / 1024 << " MiB\n";
}
}

extern "C" int run_pipeline(const char* data, const char* output, i64 start, i64 end,
                             int workers, char* error, size_t error_size) {
    try {
        if (workers < 1) throw std::runtime_error("NCPU must be positive");
        execute(data, output, start, end, workers);
        return 0;
    } catch (const std::exception& e) {
        if (error_size) std::snprintf(error, error_size, "%s", e.what());
        return 1;
    }
}
