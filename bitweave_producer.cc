// Copyright (c) 2011 The LevelDB Authors.
// BitWeaving Real-Time Producer
// Continuously writes streaming data to LevelDB (simulates sensor/log stream)

#include <iostream>
#include <iomanip>
#include <chrono>
#include <thread>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <random>
#include <atomic>
#include <signal.h>

#include "leveldb/db.h"
#include "leveldb/options.h"
#include "leveldb/write_batch.h"

using namespace leveldb;

static std::atomic<bool> keep_running(true);

void signal_handler(int sig) {
  std::cout << "\n\nShutting down producer...\n";
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
  std::cout << "BitWeaving Real-Time Producer\n";
  std::cout << "Streaming scenario: " << scenario << "\n";
  std::cout << "Database path: " << db_path << "\n";
  std::cout << "Writes per second: ~1000\n";
  std::cout << "Press Ctrl+C to stop\n";
  std::cout << std::string(80, '=') << "\n";
  
  Options opts;
  opts.create_if_missing = true;
  opts.block_size = 4096;
  
  DB* db;
  Status s = DB::Open(opts, db_path, &db);
  if (!s.ok()) {
    std::cerr << "Failed to open DB: " << s.ToString() << "\n";
    return 1;
  }
  
  WriteOptions wo;
  WriteOptions wo_sync;
  wo_sync.sync = false;  // Batch writes for speed
  
  // Get initial record count
  Iterator* init_it = db->NewIterator(ReadOptions());
  init_it->SeekToLast();
  uint64_t start_record = 0;
  if (init_it->Valid()) {
    std::string key_str = init_it->key().ToString();
    sscanf(key_str.c_str(), "key%llu", (unsigned long long*)&start_record);
    start_record++;
  }
  delete init_it;
  
  uint64_t current_record = start_record;
  uint64_t records_written = 0;
  auto start_time = std::chrono::high_resolution_clock::now();
  auto last_print = start_time;
  
  std::mt19937 gen(std::chrono::system_clock::now().time_since_epoch().count());
  
  std::cout << "\nStarting from record " << current_record << "\n";
  std::cout << "\n";
  
  while (keep_running) {
    WriteBatch batch;
    
    // Write 1000 records per batch (takes ~1 second)
    for (int i = 0; i < 1000 && keep_running; i++) {
      char key[64], val[64];
      snprintf(key, sizeof(key), "key%016llu", (unsigned long long)current_record);
      
      uint32_t value = 0;
      
      // Generate value based on scenario
      if (scenario == "temperature") {
        // Temperature: normal 20-30°C, 2% anomalies 35-50°C
        if (gen() % 100 < 2) {
          value = 35 + (gen() % 15);  // Anomaly
        } else {
          std::normal_distribution<> normal(25, 3);
          value = (uint32_t)std::max(0.0, normal(gen));
        }
      } else if (scenario == "latency") {
        // Latency: exponential, 95% normal, 5% slow
        std::exponential_distribution<> exp_dist(0.1);
        if (gen() % 100 < 5) {
          value = 500 + (gen() % 2000);  // Slow
        } else {
          value = (uint32_t)std::min(500.0, exp_dist(gen) * 10);
        }
      } else if (scenario == "cpu") {
        // CPU: normal 30-60%, 5% spikes 80-100%
        if (gen() % 100 < 5) {
          value = 80 + (gen() % 20);  // Spike
        } else {
          std::normal_distribution<> normal(45, 10);
          value = (uint32_t)std::max(0.0, std::min(100.0, normal(gen)));
        }
      } else if (scenario == "memory") {
        // Memory: linear growth with noise
        uint32_t base = 100 + (current_record / 5000);  // Grows 1MB per 5K records
        std::normal_distribution<> noise(0, 5);
        value = (uint32_t)std::max(100, (int)base + (int)noise(gen));
      }
      
      snprintf(val, sizeof(val), "%u", value);
      batch.Put(key, val);
      current_record++;
      records_written++;
    }
    
    // Write the batch
    db->Write(wo_sync, &batch);
    
    // Print progress every 10 seconds
    auto now = std::chrono::high_resolution_clock::now();
    double elapsed_sec = std::chrono::duration<double>(now - last_print).count();
    
    if (elapsed_sec >= 10.0) {
      double total_elapsed = std::chrono::duration<double>(now - start_time).count();
      double rate = records_written / total_elapsed;
      
      std::cout << std::fixed << std::setprecision(2);
      std::cout << "[" << total_elapsed << "s] Records written: " << records_written
                << " | Rate: " << rate << " rec/sec | Current record: " << current_record << "\n";
      
      last_print = now;
    }
  }
  
  delete db;
  
  auto end_time = std::chrono::high_resolution_clock::now();
  double total_time = std::chrono::duration<double>(end_time - start_time).count();
  
  std::cout << "\n" << std::string(80, '=') << "\n";
  std::cout << "Producer Stopped\n";
  std::cout << "Total records written: " << records_written << "\n";
  std::cout << "Total time: " << std::fixed << std::setprecision(2) << total_time << " seconds\n";
  std::cout << "Average rate: " << (records_written / total_time) << " records/sec\n";
  std::cout << std::string(80, '=') << "\n";
  
  return 0;
}