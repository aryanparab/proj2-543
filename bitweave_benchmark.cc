// Copyright (c) 2011 The LevelDB Authors.
// BitWeaving Evaluation Benchmark Suite
// Tests I/O reduction, latency, and recall across various workloads

#include <iostream>
#include <iomanip>
#include <chrono>
#include <vector>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <functional>

#include "leveldb/db.h"
#include "leveldb/options.h"
#include "leveldb/write_batch.h"

using namespace leveldb;

// Helper function to get temp directory
static std::string TempDir() {
  const char* tmpdir = std::getenv("TMPDIR");
  if (tmpdir) return std::string(tmpdir);
  return "/tmp";
}

struct BenchmarkResult {
  std::string name;
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
    std::cout << std::string(80, '=') << "\n";
    std::cout << "Total Records:           " << records_total << "\n";
    std::cout << "True Matches:            " << true_matches << "\n";
    std::cout << "\nWithout BitWeaving:\n";
    std::cout << "  Records Scanned:       " << records_scanned_no_bw << "\n";
    std::cout << "  Latency:               " << latency_no_bw_ms << " ms\n";
    std::cout << "\nWith BitWeaving:\n";
    std::cout << "  Records Scanned:       " << records_scanned_with_bw << "\n";
    std::cout << "  Latency:               " << latency_with_bw_ms << " ms\n";
    std::cout << "\nImprovement:\n";
    std::cout << "  I/O Reduction:         " << io_reduction_pct << "%\n";
    std::cout << "  Latency Speedup:       " 
              << (latency_no_bw_ms / latency_with_bw_ms) << "x\n";
  }
};

class BitWeavingBenchmark {
 public:
  BitWeavingBenchmark() : db_(nullptr) {}
  
  ~BitWeavingBenchmark() {
    if (db_) delete db_;
  }
  
  void Setup(const std::string& dbpath, size_t block_size = 1024) {
    // Clean up old database
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
      
      // Batch writes for efficiency
      if (batch_count >= 1000) {
        db_->Write(wo, &batch);
        batch.Clear();
        batch_count = 0;
      }
    }
    
    // Final batch
    if (batch_count > 0) {
      db_->Write(wo, &batch);
    }
  }
  
  void Compact() {
    db_->CompactRange(nullptr, nullptr);
  }
  
  BenchmarkResult ScanWithPredicate(const std::string& name, 
                                     uint32_t threshold, 
                                     char op) {
    BenchmarkResult result;
    result.name = name;
    
    // Count expected matches in-memory
    // (In a real scenario, we'd need to know the data distribution)
    // For now, we'll count from actual results
    
    // ========== Without BitWeaving ==========
    ReadOptions ro_no_bw;
    ro_no_bw.fill_cache = false;  // Don't pollute cache
    
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
    
    // ========== With BitWeaving ==========
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
    
    // Verify correctness
    if (matches != matches_with_bw) {
      std::cerr << "ERROR: Match count mismatch! " << matches << " vs " << matches_with_bw << "\n";
    }
    
    // Calculate metrics
    result.records_total = scanned_no_bw;
    result.io_reduction_pct = 100.0 * (scanned_no_bw - scanned_with_bw) / scanned_no_bw;
    
    return result;
  }
  
 private:
  DB* db_;
};

int main(int argc, char** argv) {
  std::string db_path = TempDir() + "/bitweave_benchmark";
  
  std::cout << "\n" << std::string(80, '=') << "\n";
  std::cout << "BitWeaving Evaluation Framework\n";
  std::cout << std::string(80, '=') << "\n";
  
  // ========== Benchmark 1: Uniform Distribution ==========
  {
    std::cout << "\n[1/5] Uniform Distribution (0-999)...\n";
    BitWeavingBenchmark bm;
    bm.Setup(db_path + "_uniform", 1024);
    
    bm.WriteRecords(50000, [](uint64_t i) {
      return i % 1000;  // Cyclic 0-999
    });
    bm.Compact();
    
    auto result = bm.ScanWithPredicate("Uniform: value > 800", 800, '>');
    result.Print();
  }
  
  // ========== Benchmark 2: Skewed Distribution ==========
  {
    std::cout << "\n[2/5] Skewed Distribution (most values 0-100, few > 800)...\n";
    BitWeavingBenchmark bm;
    bm.Setup(db_path + "_skewed", 1024);
    
    bm.WriteRecords(50000, [](uint64_t i) {
      if (i % 100 == 0) return 900 + (i % 100);  // 1% chance of high value
      return i % 100;  // Mostly low values
    });
    bm.Compact();
    
    auto result = bm.ScanWithPredicate("Skewed: value > 800", 800, '>');
    result.Print();
  }
  
  // ========== Benchmark 3: Two-Peak Distribution ==========
  {
    std::cout << "\n[3/5] Two-Peak Distribution (0-100 and 900-999)...\n";
    BitWeavingBenchmark bm;
    bm.Setup(db_path + "_bimodal", 1024);
    
    bm.WriteRecords(50000, [](uint64_t i) {
      return (i % 2 == 0) ? (i % 100) : (900 + (i % 100));
    });
    bm.Compact();
    
    auto result = bm.ScanWithPredicate("Bimodal: value > 800", 800, '>');
    result.Print();
  }
  
  // ========== Benchmark 4: Different Predicates ==========
  {
    std::cout << "\n[4/5] Multiple Predicates (same uniform data)...\n";
    BitWeavingBenchmark bm;
    bm.Setup(db_path + "_predicates", 1024);
    
    bm.WriteRecords(50000, [](uint64_t i) {
      return i % 1000;
    });
    bm.Compact();
    
    auto result1 = bm.ScanWithPredicate("Uniform: value > 500", 500, '>');
    result1.Print();
    
    auto result2 = bm.ScanWithPredicate("Uniform: value < 200", 200, '<');
    result2.Print();
    
    auto result3 = bm.ScanWithPredicate("Uniform: value > 900", 900, '>');
    result3.Print();
  }
  
  // ========== Benchmark 5: Scalability ==========
  {
    std::cout << "\n[5/5] Scalability (100K records)...\n";
    BitWeavingBenchmark bm;
    bm.Setup(db_path + "_scale", 1024);
    
    bm.WriteRecords(100000, [](uint64_t i) {
      return i % 1000;
    });
    bm.Compact();
    
    auto result = bm.ScanWithPredicate("Scalability: 100K records, value > 800", 800, '>');
    result.Print();
  }
  
  std::cout << "\n" << std::string(80, '=') << "\n";
  std::cout << "Evaluation Complete!\n";
  std::cout << std::string(80, '=') << "\n";
  
  return 0;
}