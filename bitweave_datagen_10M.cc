// Copyright (c) 2011 The LevelDB Authors.
// BitWeaving Controlled Data Generator - 10M Records
// Creates 10M records with temporal clustering over 100 days

#include <iostream>
#include <iomanip>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <cmath>

#include "leveldb/db.h"
#include "leveldb/options.h"
#include "leveldb/write_batch.h"

using namespace leveldb;

static std::string TempDir() {
  const char* tmpdir = std::getenv("TMPDIR");
  if (tmpdir) return std::string(tmpdir);
  return "/tmp";
}

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cerr << "Usage: " << argv[0] << " <output_path> [scenario]\n";
    std::cerr << "Scenarios: temperature (default), latency, cpu, memory\n";
    return 1;
  }
  
  std::string db_path = argv[1];
  std::string scenario = (argc > 2) ? argv[2] : "temperature";
  
  std::cout << "\n" << std::string(80, '=') << "\n";
  std::cout << "BitWeaving Controlled Data Generator - 10M Records\n";
  std::cout << "Scenario: " << scenario << "\n";
  std::cout << "Database: " << db_path << "\n";
  std::cout << "Creating 10M records with temporal clustering over 100 days...\n";
  std::cout << std::string(80, '=') << "\n\n";
  
  // Clean up old database
  DestroyDB(db_path, Options());
  
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
  wo.sync = false;
  
  uint64_t total_records = 10000000;  // 10M records instead of 50M
  uint64_t records_per_hour = 100;     // 100 records per hour
  uint64_t hours = total_records / records_per_hour;  // ~100,000 hours = ~4,166 days
  
  auto start_time = std::chrono::high_resolution_clock::now();
  auto last_print = start_time;
  
  std::cout << "Writing records with temporal clustering...\n";
  std::cout << "Span: ~100,000 hours (~4,166 days ~11 years)\n";
  std::cout << "Pattern: Daily cycles with anomalies\n\n";
  
  for (uint64_t hour = 0; hour < hours; hour++) {
    WriteBatch batch;
    
    // Simulate daily pattern: 24-hour cycle repeats
    uint32_t hour_of_day = hour % 24;
    uint32_t day_num = hour / 24;
    
    // Determine value based on time of day and scenario
    uint32_t base_value = 0;
    bool is_anomaly = false;
    
    if (scenario == "temperature") {
      // Temperature pattern: varies by hour of day
      // Night (0-6):    15-18°C
      // Morning (6-12):  18-25°C
      // Afternoon (12-18): 25-32°C
      // Evening (18-24):  20-28°C
      
      if (hour_of_day < 6) {
        base_value = 15 + hour_of_day;  // 15-21°C
      } else if (hour_of_day < 12) {
        base_value = 18 + (hour_of_day - 6) * 1.1;  // 18-25°C
      } else if (hour_of_day < 18) {
        base_value = 25 + (hour_of_day - 12) * 1.1;  // 25-32°C
      } else {
        base_value = 20 + (hour_of_day - 18);  // 20-26°C
      }
      
      // 1% of hours have anomalies (extreme heat)
      if (hour % 100 == 0) {
        is_anomaly = true;
        base_value = 40 + (hour % 15);  // 40-55°C anomaly
      }
    } else if (scenario == "latency") {
      // Latency pattern: peak during business hours
      // Off-hours (0-8, 18-24):  10-30ms
      // Business (8-18):          50-150ms (busier)
      
      if ((hour_of_day >= 8 && hour_of_day < 18)) {
        base_value = 50 + (hour_of_day - 8) * 5;  // 50-100ms
      } else {
        base_value = 10 + (hour_of_day % 10) * 2;  // 10-30ms
      }
      
      // 2% SLO violations
      if (hour % 50 == 0) {
        is_anomaly = true;
        base_value = 500 + (hour % 1000);  // 500-1500ms
      }
    } else if (scenario == "cpu") {
      // CPU pattern: low overnight, high during day
      if (hour_of_day < 6) {
        base_value = 10 + hour_of_day * 2;  // 10-22%
      } else if (hour_of_day < 18) {
        base_value = 30 + (hour_of_day - 6) * 3;  // 30-66%
      } else {
        base_value = 40 + (hour_of_day - 18) * 2;  // 40-52%
      }
      
      // 2% spikes
      if (hour % 50 == 0) {
        is_anomaly = true;
        base_value = 85 + (hour % 15);  // 85-100%
      }
    } else if (scenario == "memory") {
      // Memory: slow growth over time, with daily cycles
      uint32_t trend = 100 + (day_num / 10);  // Grows 1MB per 10 days
      uint32_t daily_var = 10 + (hour_of_day % 5);
      base_value = trend + daily_var;
    }
    
    // Write 100 records for this hour
    for (uint64_t i = 0; i < records_per_hour; i++) {
      uint64_t record_num = hour * records_per_hour + i;
      
      char key[64], val[64];
      snprintf(key, sizeof(key), "key%016llu", (unsigned long long)record_num);
      
      // Add slight variation within hour (±3)
      int32_t variation = (i % 7) - 3;
      uint32_t value = (uint32_t)std::max(0, (int32_t)base_value + variation);
      
      snprintf(val, sizeof(val), "%u", value);
      batch.Put(key, val);
    }
    
    db->Write(wo, &batch);
    
    // Progress reporting
    auto now = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(now - last_print).count();
    
    if (elapsed >= 5.0) {
      uint64_t records_written = (hour + 1) * records_per_hour;
      double total_elapsed = std::chrono::duration<double>(now - start_time).count();
      double rate = records_written / total_elapsed;
      double percent = 100.0 * records_written / total_records;
      uint64_t days_elapsed = hour / 24;
      
      std::cout << std::fixed << std::setprecision(1);
      std::cout << "[" << percent << "%] " << (records_written / 1000000.0) << "M records | "
                << rate << " rec/sec | Day " << days_elapsed << "/4166\n";
      
      last_print = now;
    }
  }
  
  std::cout << "\nCompacting database...\n";
  db->CompactRange(nullptr, nullptr);
  
  delete db;
  
  auto end_time = std::chrono::high_resolution_clock::now();
  double total_time = std::chrono::duration<double>(end_time - start_time).count();
  
  std::cout << "\n" << std::string(80, '=') << "\n";
  std::cout << "Data Generation Complete!\n";
  std::cout << "Total records: 10,000,000\n";
  std::cout << "Total time: " << std::fixed << std::setprecision(2) << total_time << " seconds\n";
  std::cout << "Average rate: " << (10000000.0 / total_time) << " records/sec\n";
  std::cout << "Database path: " << db_path << "\n";
  std::cout << "Timespan: ~4,166 days (~11 years) of hourly cycles\n";
  std::cout << std::string(80, '=') << "\n\n";
  
  std::cout << "Key properties:\n";
  std::cout << "  1. TEMPORALLY CLUSTERED: Values group by time of day\n";
  std::cout << "  2. APPEND-ONLY: Records ordered by time (like real logs)\n";
  std::cout << "  3. DAILY CYCLES: Same patterns repeat each day\n";
  std::cout << "  4. SPARSE ANOMALIES: 1-2% anomalies scattered across time\n";
  std::cout << "  5. REALISTIC: Mimics real time-series (IoT, logs, metrics)\n\n";
  
  std::cout << "Query patterns that will benefit from BitWeaving:\n";
  std::cout << "  - Range queries by time (e.g., last 7 days)\n";
  std::cout << "  - Anomaly detection (value > threshold)\n";
  std::cout << "  - Peak detection (value > 80%)\n";
  std::cout << "  - SLO violation detection (value > 500ms)\n\n";
  
  return 0;
}