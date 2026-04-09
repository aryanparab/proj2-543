// Copyright (c) 2011 The LevelDB Authors.
// BitWeaving Real-World Benchmark Suite
// Tests BitWeaving on realistic time-series, sensor, and log data

#include <iostream>
#include <iomanip>
#include <chrono>
#include <vector>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <random>

#include "leveldb/db.h"
#include "leveldb/options.h"
#include "leveldb/write_batch.h"

using namespace leveldb;

static std::string TempDir() {
  const char* tmpdir = std::getenv("TMPDIR");
  if (tmpdir) return std::string(tmpdir);
  return "/tmp";
}

struct BenchmarkResult {
  std::string name;
  std::string description;
  uint64_t records_total;
  uint64_t records_scanned_no_bw;
  uint64_t records_scanned_with_bw;
  uint64_t true_matches;
  double latency_no_bw_ms;
  double latency_with_bw_ms;
  double io_reduction_pct;
  
  void Print() const {
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "\n" << std::string(80, '=') << "\n";
    std::cout << "Benchmark: " << name << "\n";
    std::cout << "Description: " << description << "\n";
    std::cout << std::string(80, '=') << "\n";
    std::cout << "Total Records:           " << records_total << "\n";
    std::cout << "True Matches:            " << true_matches 
              << " (" << (100.0 * true_matches / records_total) << "%)\n";
    std::cout << "\nWithout BitWeaving:\n";
    std::cout << "  Records Scanned:       " << records_scanned_no_bw << "\n";
    std::cout << "  Latency:               " << latency_no_bw_ms << " ms\n";
    std::cout << "\nWith BitWeaving:\n";
    std::cout << "  Records Scanned:       " << records_scanned_with_bw << "\n";
    std::cout << "  Latency:               " << latency_with_bw_ms << " ms\n";
    std::cout << "\nImprovement:\n";
    std::cout << "  I/O Reduction:         " << io_reduction_pct << "%\n";
    if (latency_with_bw_ms > 0.01) {
      std::cout << "  Latency Speedup:       " 
                << (latency_no_bw_ms / latency_with_bw_ms) << "x\n";
    }
  }
};

class RealisticBenchmark {
 public:
  RealisticBenchmark() : db_(nullptr) {}
  
  ~RealisticBenchmark() {
    if (db_) delete db_;
  }
  
  void Setup(const std::string& dbpath, size_t block_size = 1024) {
    DestroyDB(dbpath, Options());
    
    Options opts;
    opts.create_if_missing = true;
    opts.block_size = block_size;
    
    Status s = DB::Open(opts, dbpath, &db_);
    if (!s.ok()) {
      std::cerr << "Failed to open DB: " << s.ToString() << "\n";
      exit(1);
    }
  }
  
  void WriteRecords(uint64_t count, const std::function<uint32_t(uint64_t)>& value_fn) {
    WriteOptions wo;
    WriteBatch batch;
    uint64_t batch_count = 0;
    
    for (uint64_t i = 0; i < count; i++) {
      char key[64], val[64];
      snprintf(key, sizeof(key), "key%016llu", (unsigned long long)i);
      uint32_t value = value_fn(i);
      snprintf(val, sizeof(val), "%u", value);
      
      batch.Put(key, val);
      batch_count++;
      
      if (batch_count >= 1000) {
        db_->Write(wo, &batch);
        batch.Clear();
        batch_count = 0;
      }
    }
    
    if (batch_count > 0) {
      db_->Write(wo, &batch);
    }
  }
  
  void Compact() {
    db_->CompactRange(nullptr, nullptr);
  }
  
  BenchmarkResult ScanWithPredicate(const std::string& name,
                                     const std::string& description,
                                     uint32_t threshold, 
                                     char op) {
    BenchmarkResult result;
    result.name = name;
    result.description = description;
    
    // Without BitWeaving
    ReadOptions ro_no_bw;
    ro_no_bw.fill_cache = false;
    
    auto start = std::chrono::high_resolution_clock::now();
    Iterator* it_no_bw = db_->NewIterator(ro_no_bw);
    uint64_t scanned_no_bw = 0;
    uint64_t matches = 0;
    
    for (it_no_bw->SeekToFirst(); it_no_bw->Valid(); it_no_bw->Next()) {
      scanned_no_bw++;
      uint32_t v = atoi(it_no_bw->value().data());
      
      bool match = false;
      if (op == '>') match = (v > threshold);
      else if (op == '<') match = (v < threshold);
      else if (op == '=') match = (v == threshold);
      
      if (match) matches++;
    }
    delete it_no_bw;
    auto end = std::chrono::high_resolution_clock::now();
    
    result.records_scanned_no_bw = scanned_no_bw;
    result.true_matches = matches;
    result.latency_no_bw_ms = std::chrono::duration<double, std::milli>(end - start).count();
    
    // With BitWeaving
    ReadOptions ro_with_bw;
    ro_with_bw.bw_predicate = threshold;
    ro_with_bw.bw_op = op;
    ro_with_bw.fill_cache = false;
    
    start = std::chrono::high_resolution_clock::now();
    Iterator* it_with_bw = db_->NewIterator(ro_with_bw);
    uint64_t scanned_with_bw = 0;
    uint64_t matches_with_bw = 0;
    
    for (it_with_bw->SeekToFirst(); it_with_bw->Valid(); it_with_bw->Next()) {
      scanned_with_bw++;
      uint32_t v = atoi(it_with_bw->value().data());
      
      bool match = false;
      if (op == '>') match = (v > threshold);
      else if (op == '<') match = (v < threshold);
      else if (op == '=') match = (v == threshold);
      
      if (match) matches_with_bw++;
    }
    delete it_with_bw;
    end = std::chrono::high_resolution_clock::now();
    
    result.records_scanned_with_bw = scanned_with_bw;
    result.latency_with_bw_ms = std::chrono::duration<double, std::milli>(end - start).count();
    
    if (matches != matches_with_bw) {
      std::cerr << "ERROR: Match count mismatch! " << matches << " vs " << matches_with_bw << "\n";
    }
    
    result.records_total = scanned_no_bw;
    result.io_reduction_pct = 100.0 * (scanned_no_bw - scanned_with_bw) / scanned_no_bw;
    
    return result;
  }
  
 private:
  DB* db_;
};

int main(int argc, char** argv) {
  std::string db_path = TempDir() + "/bitweave_realworld";
  
  std::cout << "\n" << std::string(80, '=') << "\n";
  std::cout << "BitWeaving Real-World Benchmark Suite\n";
  std::cout << "Testing on realistic time-series and sensor data\n";
  std::cout << std::string(80, '=') << "\n";
  
  // ========== Benchmark 1: IoT Temperature Sensor ==========
  {
    std::cout << "\n[1/6] IoT Temperature Sensor Data...\n";
    RealisticBenchmark bm;
    bm.Setup(db_path + "_iot_temp", 1024);
    
    // Temperature sensor readings: 15-35°C with occasional spikes
    bm.WriteRecords(100000, [](uint64_t i) {
      static std::mt19937 gen(42);
      static std::normal_distribution<> normal_dist(25, 3);  // Mean 25°C, StdDev 3°C
      static std::uniform_real_distribution<> spike_dist(0, 1);
      
      // 98% normal readings, 2% anomalies (>35°C)
      if (spike_dist(gen) < 0.02) {
        return 35 + (uint32_t)(spike_dist(gen) * 20);  // 35-55°C anomalies
      }
      return (uint32_t)std::max(0.0, normal_dist(gen));
    });
    bm.Compact();
    
    // Query: Find anomalies (>35°C)
    auto result = bm.ScanWithPredicate(
      "IoT Temp Anomaly Detection",
      "Temperature sensor: detect readings > 35°C (anomalies)",
      35, '>');
    result.Print();
  }
  
  // ========== Benchmark 2: Network Latency Logs ==========
  {
    std::cout << "\n[2/6] Network Latency Logs...\n";
    RealisticBenchmark bm;
    bm.Setup(db_path + "_network_latency", 1024);
    
    // Network latency: typically 10-50ms, occasional spikes >500ms
    bm.WriteRecords(100000, [](uint64_t i) {
      static std::mt19937 gen(43);
      static std::gamma_distribution<> gamma_dist(2.0, 10.0);  // Right-skewed
      
      // 95% normal, 5% slow responses >500ms
      if (gen() % 100 < 5) {
        return 500 + (gen() % 2000);  // 500-2500ms slow
      }
      return (uint32_t)std::min(500.0, gamma_dist(gen));
    });
    bm.Compact();
    
    // Query: Find slow requests (>500ms)
    auto result = bm.ScanWithPredicate(
      "Network Latency SLO Violation",
      "Network logs: detect requests > 500ms (SLO violation)",
      500, '>');
    result.Print();
  }
  
  // ========== Benchmark 3: CPU Usage Monitoring ==========
  {
    std::cout << "\n[3/6] CPU Usage Monitoring...\n";
    RealisticBenchmark bm;
    bm.Setup(db_path + "_cpu_usage", 1024);
    
    // CPU usage: 0-100%, normally 20-60%, spikes to 100%
    bm.WriteRecords(100000, [](uint64_t i) {
      static std::mt19937 gen(44);
      static std::normal_distribution<> normal_dist(40, 10);  // Normal: 40%
      
      // 95% normal operation, 5% high load >80%
      if (gen() % 100 < 5) {
        return 80 + (gen() % 20);  // 80-100% high load
      }
      return (uint32_t)std::max(0.0, std::min(100.0, normal_dist(gen)));
    });
    bm.Compact();
    
    // Query: Find high-load periods (>80%)
    auto result = bm.ScanWithPredicate(
      "CPU High Load Detection",
      "CPU monitoring: detect periods > 80% utilization",
      80, '>');
    result.Print();
  }
  
  // ========== Benchmark 4: Database Query Response Times ==========
  {
    std::cout << "\n[4/6] Database Query Response Times...\n";
    RealisticBenchmark bm;
    bm.Setup(db_path + "_db_response", 1024);
    
    // Query times: exponentially distributed (typical for DB)
    bm.WriteRecords(100000, [](uint64_t i) {
      static std::mt19937 gen(45);
      static std::exponential_distribution<> exp_dist(0.1);  // Mean 10ms
      
      // Realistic: most fast, some slow, few very slow
      return (uint32_t)std::min(5000.0, exp_dist(gen) * 10);  // 0-5000ms
    });
    bm.Compact();
    
    // Query: Find slow queries (>100ms)
    auto result = bm.ScanWithPredicate(
      "Database Slow Query Detection",
      "DB logs: detect queries > 100ms (slow)",
      100, '>');
    result.Print();
  }
  
  // ========== Benchmark 5: Application Memory Usage ==========
  {
    std::cout << "\n[5/6] Application Memory Usage...\n";
    RealisticBenchmark bm;
    bm.Setup(db_path + "_memory_usage", 1024);
    
    // Memory: starts low, grows over time (memory leak pattern)
    bm.WriteRecords(100000, [](uint64_t i) {
      // Simulate memory leak: starts at 100MB, grows to 500MB
      uint32_t base = 100 + (i / 200);  // Grows 1MB per 200 records
      static std::mt19937 gen(46);
      static std::normal_distribution<> noise(0, 5);  // ±5MB noise
      return (uint32_t)std::max(100, (int)base + (int)noise(gen));
    });
    bm.Compact();
    
    // Query: Find high memory usage (>400MB - potential memory leak)
    auto result = bm.ScanWithPredicate(
      "Memory Leak Detection",
      "App monitoring: detect memory > 400MB (potential leak)",
      400, '>');
    result.Print();
  }
  
  // ========== Benchmark 6: Disk I/O Throughput ==========
  {
    std::cout << "\n[6/6] Disk I/O Throughput...\n";
    RealisticBenchmark bm;
    bm.Setup(db_path + "_disk_io", 1024);
    
    // Disk I/O: periodic spikes (backups, syncs)
    bm.WriteRecords(100000, [](uint64_t i) {
      // Every 10000 records, there's a spike (backup/sync)
      if ((i % 10000) < 1000) {
        return 500 + (i % 500);  // 500-1000 MB/s during spikes
      }
      return 50 + (i % 100);  // 50-150 MB/s normal
    });
    bm.Compact();
    
    // Query: Find high I/O periods (>300 MB/s)
    auto result = bm.ScanWithPredicate(
      "High Disk I/O Detection",
      "I/O logs: detect throughput > 300 MB/s (unusual)",
      300, '>');
    result.Print();
  }
  
  std::cout << "\n" << std::string(80, '=') << "\n";
  std::cout << "Real-World Evaluation Complete!\n";
  std::cout << "\nKey Insights:\n";
  std::cout << "- Anomaly detection queries benefit most from BitWeaving\n";
  std::cout << "- Time-series data often has temporal clustering (values in order)\n";
  std::cout << "- BitWeaving excels at filtering 'normal' blocks in skewed distributions\n";
  std::cout << "- Query selectivity (% matches) determines I/O reduction\n";
  std::cout << std::string(80, '=') << "\n";
  
  return 0;
}