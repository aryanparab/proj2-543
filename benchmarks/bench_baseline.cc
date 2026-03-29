#include <iostream>
#include <chrono>
#include <string>
#include <vector>
#include <random>
#include <cassert>
#include <filesystem>

#include "leveldb/db.h"
#include "leveldb/options.h"
#include "leveldb/write_batch.h"

// ─── helpers ────────────────────────────────────────────────────────────────

static leveldb::DB* openDB(const std::string& path) {
    leveldb::DB* db;
    leveldb::Options opts;
    opts.create_if_missing = true;
    opts.write_buffer_size = 64 * 1024 * 1024; // 64 MB memtable
    leveldb::Status s = leveldb::DB::Open(opts, path, &db);
    assert(s.ok());
    return db;
}

static void destroyDB(const std::string& path) {
    leveldb::Options opts;
    leveldb::DestroyDB(path, opts);
}

// ─── data generators ────────────────────────────────────────────────────────

// Temporally coherent: values drift slowly over time (IoT/sensor pattern)
// Groups of records will naturally fall into similar value bands
std::vector<std::pair<std::string, int>> makeCoherentData(int n) {
    std::vector<std::pair<std::string, int>> data;
    data.reserve(n);
    std::mt19937 rng(42);
    std::normal_distribution<float> noise(0, 15);

    // Simulate 3 distinct "phases" of sensor readings
    // Phase 1: low values (200-400)  — first third
    // Phase 2: high values (850-950) — second third  
    // Phase 3: mid values (400-600)  — last third
    // This gives BitWeaving clear groups to skip

    struct Phase { float center; int start; int end; };
    std::vector<Phase> phases = {
        {300.0f, 0,         n/3},
        {900.0f, n/3,       2*n/3},
        {500.0f, 2*n/3,     n}
    };

    for (auto& phase : phases) {
        float base = phase.center;
        for (int i = phase.start; i < phase.end; i++) {
            base += (rng() % 2 == 0 ? 1 : -1) * (rng() % 3);
            base = std::max((float)(phase.center - 100), 
                           std::min((float)(phase.center + 100), base));
            int val = (int)(base + noise(rng));
            val = std::max(0, std::min(1023, val));

            char key[32];
            snprintf(key, sizeof(key), "sensor:%08d", i);
            data.push_back({key, val});
        }
    }
    return data;
}
// Random: values are uniformly random — no temporal coherence
std::vector<std::pair<std::string, int>> makeRandomData(int n) {
    std::vector<std::pair<std::string, int>> data;
    data.reserve(n);
    std::mt19937 rng(99);
    std::uniform_int_distribution<int> dist(0, 1023);

    for (int i = 0; i < n; i++) {
        char key[32];
        snprintf(key, sizeof(key), "sensor:%08d", i);
        data.push_back({key, dist(rng)});
    }
    return data;
}

// ─── write phase ─────────────────────────────────────────────────────────────

static void writeData(leveldb::DB* db,
                      const std::vector<std::pair<std::string, int>>& data) {
    leveldb::WriteBatch batch;
    int count = 0;
    for (auto& [key, val] : data) {
        batch.Put(key, std::to_string(val));
        count++;
        if (count % 1000 == 0) {
            leveldb::Status s = db->Write(leveldb::WriteOptions(), &batch);
            assert(s.ok());
            batch.Clear();
        }
    }
    // flush remainder
    if (count % 1000 != 0) {
        db->Write(leveldb::WriteOptions(), &batch);
    }

    // Force compaction so data is in SST files (not just memtable)
    db->CompactRange(nullptr, nullptr);
}

// ─── scan benchmark ──────────────────────────────────────────────────────────

struct ScanResult {
    long long latency_us;   // microseconds
    int records_visited;
    int records_matched;
};

static ScanResult runScan(leveldb::DB* db, int predicate) {
    leveldb::ReadOptions ro;
    // No BitWeaving yet — stock LevelDB scan
    
    int visited = 0, matched = 0;

    auto t0 = std::chrono::high_resolution_clock::now();

    leveldb::Iterator* it = db->NewIterator(ro);
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        visited++;
        int val = std::stoi(it->value().ToString());
        if (val > predicate) matched++;
    }
    delete it;

    auto t1 = std::chrono::high_resolution_clock::now();
    long long us = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();

    return {us, visited, matched};
}

// ─── runner ──────────────────────────────────────────────────────────────────

static void runBenchmark(const std::string& label,
                         const std::string& dbpath,
                         const std::vector<std::pair<std::string, int>>& data,
                         int predicate) {
    destroyDB(dbpath);
    leveldb::DB* db = openDB(dbpath);

    std::cout << "\n=== " << label << " ===\n";
    std::cout << "Writing " << data.size() << " records...\n";

    auto tw0 = std::chrono::high_resolution_clock::now();
    writeData(db, data);
    auto tw1 = std::chrono::high_resolution_clock::now();
    long long write_us = std::chrono::duration_cast<std::chrono::microseconds>(tw1 - tw0).count();
    std::cout << "Write + compaction: " << write_us / 1000 << " ms\n";

    std::cout << "Running scan (predicate: value > " << predicate << ")...\n";

    // Run 5 times and take average to reduce noise
    long long total_us = 0;
    int visited = 0, matched = 0;
    int runs = 5;
    for (int i = 0; i < runs; i++) {
        auto r = runScan(db, predicate);
        total_us += r.latency_us;
        visited = r.records_visited;
        matched = r.records_matched;
    }

    std::cout << "Avg scan latency : " << total_us / runs / 1000 << " ms ("
              << total_us / runs << " us)\n";
    std::cout << "Records visited  : " << visited << "\n";
    std::cout << "Records matched  : " << matched << "\n";
    std::cout << "Selectivity      : "
              << (100.0 * matched / visited) << "%\n";

    delete db;
    destroyDB(dbpath);
}

// ─── main ────────────────────────────────────────────────────────────────────

int main() {
    const int N = 100000;   // 100k records
    const int PREDICATE = 800; // value > 800

    std::cout << "Generating datasets (" << N << " records each)...\n";
    auto coherent = makeCoherentData(N);
    auto random   = makeRandomData(N);

    runBenchmark("COHERENT DATA (stock LevelDB baseline)",
                 "/tmp/bench_coherent", coherent, PREDICATE);

    runBenchmark("RANDOM DATA (stock LevelDB baseline)",
                 "/tmp/bench_random", random, PREDICATE);

    std::cout << "\nDone. Save these numbers — they are your baseline.\n";
    return 0;
}