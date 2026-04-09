// Copyright (c) 2011 The LevelDB Authors.
// BitWeaving Real-Time Consumer with Date-Range Queries
// Continuously queries LevelDB by date range (temporal queries)
// FIXED: Uses snapshots for concurrent read/write

#include <iostream>
#include <iomanip>
#include <chrono>
#include <thread>
#include <cstdlib>
#include <cstring>
#include <atomic>
#include <signal.h>
#include <ctime>

#include "leveldb/db.h"
#include "leveldb/options.h"

using namespace leveldb;

static std::atomic<bool> keep_running(true);

void signal_handler(int sig) {
  std::cout << "\n\nShutting down consumer...\n";
  keep_running = false;
}

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cerr << "Usage: " << argv[0] << " <db_path> [scenario]\n";
    std::cerr << "Scenarios: temperature (default), latency, cpu, memory\n";
    return 1;
  }
  
  std::string db_path = argv[1];
  std::string scenario = (argc > 2) ? argv[2] : "temperature";
  
  // Register signal handler for graceful shutdown
  signal(SIGINT, signal_handler);
  signal(SIGTERM, signal_handler);
  
  std::cout << "\n" << std::string(80, '=') << "\n";
  std::cout << "BitWeaving Real-Time Consumer (Date-Range Queries)\n";
  std::cout << "Streaming scenario: " << scenario << "\n";
  std::cout << "Database path: " << db_path << "\n";
  std::cout << "Query interval: 5 seconds\n";
  std::cout << "Query type: DATE RANGE + VALUE PREDICATE\n";
  std::cout << "Press Ctrl+C to stop\n";
  std::cout << std::string(80, '=') << "\n";
  
  // Wait for producer to initialize database
  std::cout << "Waiting for producer to initialize database...\n";
  std::cout << "Retrying every 500ms...\n";
  
  Options opts;
  opts.block_size = 4096;
  
  DB* db = nullptr;
  Status s;
  
  // Retry loop: wait for producer to create database
  for (int attempt = 0; attempt < 60; attempt++) {  // 30 seconds
    s = DB::Open(opts, db_path, &db);
    if (s.ok()) {
      std::cout << "✓ Connected to database on attempt " << (attempt + 1) << "\n";
      break;
    }
    
    if (attempt % 4 == 0) {
      std::cout << "  Waiting... (attempt " << (attempt + 1) << "/60)\n";
    }
    
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }
  
  if (!s.ok()) {
    std::cerr << "Failed to connect to database after 30 seconds!\n";
    std::cerr << "Error: " << s.ToString() << "\n";
    return 1;
  }
  
  uint32_t threshold = 0;
  char op = '>';
  std::string description;
  
  if (scenario == "temperature") {
    threshold = 35;
    op = '>';
    description = "Temperature anomalies (>35°C) in last 24 hours";
  } else if (scenario == "latency") {
    threshold = 500;
    op = '>';
    description = "Slow requests (>500ms) in last 24 hours";
  } else if (scenario == "cpu") {
    threshold = 80;
    op = '>';
    description = "High CPU load (>80%) in last 24 hours";
  } else if (scenario == "memory") {
    threshold = 400;
    op = '>';
    description = "Memory usage (>400MB) in last 24 hours";
  }
  
  std::cout << "Query: " << description << "\n";
  std::cout << "Query Strategy: Scan all records, filter by (time AND value)\n";
  std::cout << std::string(80, '=') << "\n\n";
  
  uint64_t query_count = 0;
  auto start_time = std::chrono::high_resolution_clock::now();
  
  // Track query windows
  uint64_t hour_window = 24;  // Query last 24 hours of data
  uint64_t current_hour = 0;
  
  while (keep_running) {
    // Sleep 5 seconds between queries
    std::this_thread::sleep_for(std::chrono::seconds(5));
    
    // Simulate sliding window: query different 24-hour periods
    current_hour = (query_count * 6) % 4166;  // Cycle through all days
    uint64_t start_hour = current_hour;
    uint64_t end_hour = current_hour + hour_window;
    
    // Create snapshot for consistent read while producer writes
    const Snapshot* snapshot = db->GetSnapshot();
    
    // ========== WITHOUT BitWeaving ==========
    ReadOptions ro_no_bw;
    ro_no_bw.snapshot = snapshot;
    ro_no_bw.fill_cache = false;
    
    Iterator* it_no_bw = db->NewIterator(ro_no_bw);
    uint64_t scanned_no_bw = 0;
    uint64_t matches_no_bw = 0;
    
    auto bw_start = std::chrono::high_resolution_clock::now();
    for (it_no_bw->SeekToFirst(); it_no_bw->Valid(); it_no_bw->Next()) {
      uint64_t record_num = atoll(it_no_bw->key().ToString().substr(3).c_str());
      uint32_t v = atoi(it_no_bw->value().data());
      
      // Extract hour from record number (100 records per hour)
      uint64_t hour = record_num / 100;
      
      // Filter by time window AND value
      bool in_time_range = (hour >= start_hour && hour <= end_hour);
      bool matches_value = false;
      
      if (op == '>') matches_value = (v > threshold);
      else if (op == '<') matches_value = (v < threshold);
      
      scanned_no_bw++;
      if (in_time_range && matches_value) {
        matches_no_bw++;
      }
    }
    delete it_no_bw;
    auto bw_end = std::chrono::high_resolution_clock::now();
    double latency_no_bw = std::chrono::duration<double, std::milli>(bw_end - bw_start).count();
    
    // ========== WITH BitWeaving ==========
    ReadOptions ro_with_bw;
    ro_with_bw.snapshot = snapshot;
    ro_with_bw.bw_predicate = threshold;
    ro_with_bw.bw_op = op;
    ro_with_bw.fill_cache = false;
    
    Iterator* it_with_bw = db->NewIterator(ro_with_bw);
    uint64_t scanned_with_bw = 0;
    uint64_t matches_with_bw = 0;
    
    bw_start = std::chrono::high_resolution_clock::now();
    for (it_with_bw->SeekToFirst(); it_with_bw->Valid(); it_with_bw->Next()) {
      uint64_t record_num = atoll(it_with_bw->key().ToString().substr(3).c_str());
      uint32_t v = atoi(it_with_bw->value().data());
      
      // Extract hour from record number
      uint64_t hour = record_num / 100;
      
      // Filter by time window AND value
      bool in_time_range = (hour >= start_hour && hour <= end_hour);
      bool matches_value = false;
      
      if (op == '>') matches_value = (v > threshold);
      else if (op == '<') matches_value = (v < threshold);
      
      scanned_with_bw++;
      if (in_time_range && matches_value) {
        matches_with_bw++;
      }
    }
    delete it_with_bw;
    bw_end = std::chrono::high_resolution_clock::now();
    double latency_with_bw = std::chrono::duration<double, std::milli>(bw_end - bw_start).count();
    
    // Release snapshot
    db->ReleaseSnapshot(snapshot);
    
    double io_reduction = 100.0 * (scanned_no_bw - scanned_with_bw) / scanned_no_bw;
    double speedup = latency_no_bw / (latency_with_bw > 0.01 ? latency_with_bw : 0.01);
    
    query_count++;
    
    // Print result
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "[Query #" << query_count << "] ";
    std::cout << "Hours: " << start_hour << "-" << end_hour << " | ";
    std::cout << "Records scanned: " << scanned_no_bw << " | ";
    std::cout << "Matches: " << matches_with_bw << " | ";
    std::cout << "I/O Reduction: " << io_reduction << "% | ";
    std::cout << "Speedup: " << speedup << "x | ";
    std::cout << "Time: " << latency_with_bw << "ms\n";
    
    // Verify correctness
    if (matches_no_bw != matches_with_bw) {
      std::cerr << "ERROR: Match mismatch! " << matches_no_bw << " vs " << matches_with_bw << "\n";
    }
  }
  
  delete db;
  
  auto end_time = std::chrono::high_resolution_clock::now();
  double total_time = std::chrono::duration<double>(end_time - start_time).count();
  
  std::cout << "\n" << std::string(80, '=') << "\n";
  std::cout << "Consumer Stopped\n";
  std::cout << "Total queries: " << query_count << "\n";
  std::cout << "Total time: " << std::fixed << std::setprecision(2) << total_time << " seconds\n";
  std::cout << "Average query rate: " << (query_count / total_time) << " queries/sec\n";
  std::cout << std::string(80, '=') << "\n";
  
  return 0;
}