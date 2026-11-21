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
    // Each parser owns its buffers. The second dimension separates event and click IDs.
    std::vector<std::vector<std::vector<Parsed>>> parsed(workers,
        std::vector<std::vector<Parsed>>(shard_count * 2));
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
                    const auto category = products.find(f[5]);
                    row.code = category == products.end() ? 1 : category->second;
                }
            }
            const size_t shard = std::hash<std::string_view>{}(row.id) % shard_count + (chunk.click ? shard_count : 0);
            parsed[worker][shard].push_back(row);
        }
    });
    const auto parsed_at = std::chrono::steady_clock::now();
    std::cerr << "Native chunk parsing: " << std::chrono::duration<double>(parsed_at - began).count() << " seconds\n";

    std::vector<std::vector<std::vector<Parsed*>>> buckets(shard_count * 2,
        std::vector<std::vector<Parsed*>>(shard_count));
    parallel(shard_count * 2, workers, [&](size_t shard, size_t) {
        size_t count = 0;
        for (const auto& worker : parsed) count += worker[shard].size();
        // Keys reference mapped input bytes. Equality compares full IDs after hashing.
        std::unordered_map<std::string_view, Parsed*> winners;
        winners.reserve(count);
        for (auto& worker : parsed) {
            for (auto& row : worker[shard]) {
                auto [it, added] = winners.try_emplace(row.id, &row);
                if (!added && row.ingest < it->second->ingest) it->second = &row;
            }
        }
        for (auto& [id, row] : winners) {
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
        std::unordered_map<std::string_view, std::uint32_t> person_codes;
        person_codes.reserve(count / 20 + 1);
        std::vector<std::string_view> persons, click_ids;
        std::vector<CompactRow> rows;
        rows.reserve(count);
        for (auto& source : buckets) {
            for (const auto* row : source[shard]) {
                if (persons.size() >= std::numeric_limits<std::uint32_t>::max()) throw std::runtime_error("Too many people in a shard");
                auto [person, added] = person_codes.try_emplace(row->user, persons.size());
                if (added) persons.push_back(row->user);
                std::uint32_t click = 0;
                if (row->code & CLICK) {
                    if (click_ids.size() >= std::numeric_limits<std::uint32_t>::max()) throw std::runtime_error("Too many clicks in a shard");
                    click_ids.push_back(row->id);
                    click = click_ids.size();
                }
                rows.push_back({row->ts, person->second, row->code & ~CLICK, row->revenue, click});
            }
            std::vector<Parsed*>().swap(source[shard]);
        }
        std::sort(rows.begin(), rows.end(), [&](const auto& a, const auto& b) {
            if (a.person != b.person) return a.person < b.person;
            if (a.ts != b.ts) return a.ts < b.ts;
            if (bool(a.click) != bool(b.click)) return bool(a.click);
            if (a.click && b.click) return click_ids[a.click - 1] < click_ids[b.click - 1];
            return false;
        });
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
